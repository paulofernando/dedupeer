package com.dedupeer.gui.component;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GradientPaint;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;

import javax.swing.JComponent;

import com.dedupeer.utils.Range;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class ModificationBar extends JComponent {

	private static final long serialVersionUID = 3032628316015934754L;
		
	private Color[] colorNormalArea = new Color[]{new Color(0xf1f0f0), new Color(0xd8d8d8)};
	private Color[] colorsModifiedArea = new Color[]{ new Color(0x4ea5d2), new Color(0x078ace) };
	
	private ArrayList<Range> ranges;
	private long fileLength;
		
	private int componenteHeight = 40;
	
	public ModificationBar(ArrayList<Range> ranges, long fileLength) {
		super();
		this.setPreferredSize(new Dimension(this.getWidth(), componenteHeight));		
		this.ranges = ranges;
		this.fileLength = fileLength;
		
		addComponentListener(new ComponentAdapter() {
		    @Override
		    public void componentResized(ComponentEvent e) {
		        setSize(new Dimension(ModificationBar.this.getWidth(), componenteHeight));
		        super.componentResized(e);
		    }
		});
	}
	
	@Override
	public void paintComponent(Graphics g) {
		 Graphics2D g2 = (Graphics2D) g;
		 g2.fillRect(0, 0, this.getWidth(), componenteHeight);
		 Rectangle2D r = new Rectangle2D.Double(0, 0, this.getWidth(), componenteHeight);
         GradientPaint gp = new GradientPaint(0, 0, colorNormalArea[0], 0, this.getHeight(), colorNormalArea[1], true);
         g2.setPaint(gp);
         g2.fill(r);
		 
		 for(Range range: ranges) {
			 int intialPosition = (int) ((((range.getInitialValue()*100)/fileLength)/100f) * this.getWidth());
			 int finalPosition = (int) ((((range.getFinalValue()*100)/fileLength)/100f) * this.getWidth());
			 
			 if(finalPosition - intialPosition > 0) {
				 r = new Rectangle2D.Double(intialPosition, 0, finalPosition - intialPosition, componenteHeight);
		         gp = new GradientPaint(0, 0, colorsModifiedArea[0], 0, this.getHeight(), colorsModifiedArea[1], true);
		         g2.setPaint(gp);
		         g2.fill(r);			 
			 }
		 }
		 
		 g2.setColor(new Color(0x626262));
		 g2.drawLine(0, this.getHeight() - 1, this.getWidth(), this.getHeight() - 1);
	}	
}

